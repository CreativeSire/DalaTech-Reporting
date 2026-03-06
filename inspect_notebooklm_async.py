import asyncio
from notebooklm import NotebookLMClient

def inspect_obj(obj, name):
    print(f"\n--- {name} methods ---")
    for m in dir(obj):
        if not m.startswith('_'):
            print(f"- {m}")

async def main():
    try:
        # Use from_storage to avoid requiring auth object directly
        client = await NotebookLMClient.from_storage()
        inspect_obj(client, "NotebookLMClient")
        
        # Check for managers
        managers = ['notebooks', 'sources', 'artifacts', 'notes', 'chat', 'sharing', 'research']
        for manager_name in managers:
            if hasattr(client, manager_name):
                manager = getattr(client, manager_name)
                inspect_obj(manager, f"client.{manager_name}")
        
        await client.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
