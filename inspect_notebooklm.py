from notebooklm import NotebookLMClient

def inspect_obj(obj, name):
    print(f"\n--- {name} methods ---")
    for m in dir(obj):
        if not m.startswith('_'):
            print(f"- {m}")

try:
    # Use from_storage to avoid requiring auth object directly
    # It will use the default path if not provided
    client = NotebookLMClient.from_storage()
    inspect_obj(client, "NotebookLMClient")
    
    # Check for managers
    managers = ['notebooks', 'sources', 'artifacts', 'notes', 'chat', 'sharing', 'research']
    for manager_name in managers:
        if hasattr(client, manager_name):
            manager = getattr(client, manager_name)
            inspect_obj(manager, f"client.{manager_name}")

except Exception as e:
    print(f"Error: {e}")
