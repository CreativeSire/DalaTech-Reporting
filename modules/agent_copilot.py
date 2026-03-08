"""
agent_copilot.py — approval-first copilot helpers for DALA Analytics.
"""

from __future__ import annotations

import json
from datetime import datetime

from .narrative_ai import gemini_available, _get_client  # type: ignore


def _metric_direction(value: float | None, positive='Growing', negative='Declining'):
    if value is None:
        return 'N/A'
    if value >= 5:
        return positive
    if value <= -5:
        return negative
    return 'Stable'


def build_default_agent_actions(ds, report=None):
    report = report or ds.get_latest_report()
    if not report:
        return []

    report_id = report['id']
    created = []
    brand_rows = ds.get_all_brand_kpis(report_id)
    forecasts = {}
    try:
        from .predictor import build_brand_forecasts

        histories = {row['brand_name']: list(reversed(ds.get_brand_history(row['brand_name'], limit=12)))
                     for row in brand_rows}
        forecasts = build_brand_forecasts(histories)
    except Exception:
        forecasts = {}

    for row in brand_rows:
        brand = row['brand_name']
        signature_base = f"{report_id}:{brand}"
        if row.get('stock_days_cover', 0) and float(row.get('stock_days_cover') or 0) <= 7:
            created.append(ds.create_agent_action(
                agent_type='Brand Health Agent',
                subject_type='brand',
                subject_key=brand,
                report_id=report_id,
                priority='high',
                title=f'Protect {brand} stock position',
                reason=f"Stock cover is down to {float(row.get('stock_days_cover') or 0):.0f} days.",
                proposed_payload={
                    'kind': 'stock_follow_up',
                    'brand_name': brand,
                    'stock_days_cover': row.get('stock_days_cover'),
                },
                action_signature=f'{signature_base}:stock_protect',
            ))

        fc = forecasts.get(ds.analytics_brand_name(brand), {})
        if fc and fc.get('growth_label') == 'Declining':
            created.append(ds.create_agent_action(
                agent_type='Forecast Agent',
                subject_type='brand',
                subject_key=brand,
                report_id=report_id,
                priority='medium',
                title=f'Review {brand} revenue decline',
                reason=f"Forecast trend is declining with {fc.get('confidence_band', 'unknown')} confidence.",
                proposed_payload={
                    'kind': 'forecast_review',
                    'brand_name': brand,
                    'forecast': fc.get('forecast'),
                    'pct_change': fc.get('pct_change'),
                    'confidence_band': fc.get('confidence_band'),
                },
                action_signature=f'{signature_base}:forecast_decline',
            ))

        if row.get('repeat_pct', 0) and float(row.get('repeat_pct') or 0) < 30:
            created.append(ds.create_agent_action(
                agent_type='Brand Health Agent',
                subject_type='brand',
                subject_key=brand,
                report_id=report_id,
                priority='medium',
                title=f'Improve repeat ordering for {brand}',
                reason=f"Repeat purchase rate is {float(row.get('repeat_pct') or 0):.1f}%.",
                proposed_payload={
                    'kind': 'repeat_rate_follow_up',
                    'brand_name': brand,
                    'repeat_pct': row.get('repeat_pct'),
                },
                action_signature=f'{signature_base}:repeat_rate',
            ))

    pending_reviews = ds.get_catalog_review_queue(status='pending', limit=10)
    if pending_reviews:
        created.append(ds.create_agent_action(
            agent_type='Data Quality Agent',
            subject_type='catalog',
            subject_key='pending_reviews',
            report_id=report_id,
            priority='high',
            title='Resolve pending brand and SKU review items',
            reason=f'{len(pending_reviews)} catalog review item(s) are waiting for decision.',
            proposed_payload={
                'kind': 'catalog_review',
                'pending_count': len(pending_reviews),
            },
            action_signature=f'{report_id}:catalog_reviews',
        ))

    activity_summary = ds.get_activity_summary(report_id=report_id)
    if activity_summary['totals']['issues'] >= 5:
        top_issue = activity_summary['top_issues'][0]['issue_type'] if activity_summary['top_issues'] else 'field issues'
        created.append(ds.create_agent_action(
            agent_type='Activity Agent',
            subject_type='activity_batch',
            subject_key=str(report_id),
            report_id=report_id,
            priority='high',
            title='Review repeated field issues from latest activity batch',
            reason=f"{activity_summary['totals']['issues']} issues logged. Top theme: {top_issue}.",
            proposed_payload={
                'kind': 'activity_issue_review',
                'top_issue': top_issue,
                'issue_total': activity_summary['totals']['issues'],
            },
            action_signature=f'{report_id}:activity_issue_review',
        ))

    return [action for action in created if action]


def _compose_context(ds, report=None, brand_name=None) -> dict:
    report = report or ds.get_latest_report()
    if not report:
        return {'report': None}
    report_id = report['id']
    all_kpis = ds.get_all_brand_kpis(report_id)
    top_brand = max(all_kpis, key=lambda row: row.get('total_revenue', 0), default=None)
    activity_summary = ds.get_activity_summary(report_id=report_id)
    actions = ds.list_agent_actions(status='pending', limit=8)
    context = {
        'report': report,
        'brand_name': brand_name,
        'portfolio': {
            'brand_count': len(all_kpis),
            'total_revenue': round(sum(row.get('total_revenue', 0) for row in all_kpis), 2),
            'top_brand': top_brand['brand_name'] if top_brand else None,
            'top_brand_revenue': round(top_brand.get('total_revenue', 0), 2) if top_brand else 0,
        },
        'activity': activity_summary,
        'pending_actions': actions,
        'alerts': ds.get_alerts(report_id=report_id, unacknowledged_only=True)[:8],
    }
    if brand_name:
        context['brand'] = {
            'kpis': ds.get_brand_kpis_single(report_id, brand_name),
            'history': ds.get_brand_history(brand_name, limit=6),
            'activity': ds.get_activity_brand_summary(brand_name),
        }
    return context


def _deterministic_answer(question: str, context: dict) -> str:
    report = context.get('report') or {}
    if not report:
        return 'No report data is available yet.'
    brand_ctx = context.get('brand')
    if brand_ctx and brand_ctx.get('kpis'):
        kpis = brand_ctx['kpis']
        activity = brand_ctx.get('activity') or {}
        return (
            f"{context['brand_name']} in {report.get('month_label')} recorded ₦{float(kpis.get('total_revenue', 0)):,.2f} "
            f"across {int(kpis.get('num_stores', 0))} supermarkets with a repeat rate of {float(kpis.get('repeat_pct', 0)):.1f}%. "
            f"Recent field activity logged {activity.get('mentions', 0)} brand mentions across "
            f"{activity.get('stores', 0)} stores. The immediate priorities are stock position, repeat ordering, "
            f"and resolving any repeated field issues."
        )
    portfolio = context.get('portfolio') or {}
    activity = context.get('activity', {}).get('totals', {})
    return (
        f"DALA currently has {portfolio.get('brand_count', 0)} active brands in {report.get('month_label')}, "
        f"with total revenue of ₦{float(portfolio.get('total_revenue', 0)):,.2f}. "
        f"Top performer is {portfolio.get('top_brand') or 'N/A'}. "
        f"The latest activity batch recorded {activity.get('visits', 0)} visits and {activity.get('issues', 0)} issues. "
        f"There are {len(context.get('pending_actions', []))} pending agent actions to review."
    )


def answer_admin_query(ds, question: str, report=None, brand_name=None) -> dict:
    question = str(question or '').strip()
    context = _compose_context(ds, report=report, brand_name=brand_name)
    memories = ds.search_agent_memories(question, limit=6)
    context['memories'] = memories

    answer = None
    used_gemini = False
    if gemini_available():
        try:
            client = _get_client()
            prompt = (
                "You are the DALA Admin Copilot. Answer as a concise business operator, not as an AI assistant. "
                "Use the context provided. Focus on practical next steps, risks, and opportunities.\n\n"
                f"Question: {question}\n"
                f"Context JSON: {json.dumps(context, default=str)}\n\n"
                "Return 1 short paragraph and, if useful, 3 crisp bullet points. Do not mention AI."
            )
            answer = client.generate_content(prompt).text.strip()
            used_gemini = True
        except Exception:
            answer = None

    if not answer:
        answer = _deterministic_answer(question, context)

    ds.save_agent_memory(
        scope_type='copilot_query',
        scope_key=datetime.now().strftime('%Y-%m'),
        memory_text=f"Q: {question}\nA: {answer[:600]}",
        memory_kind='copilot_session',
        confidence=0.65 if used_gemini else 0.55,
        source='admin_copilot',
    )
    return {
        'answer': answer,
        'used_gemini': used_gemini,
        'context': context,
        'memories': memories,
    }

