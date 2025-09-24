import yaml

def load_config(file_path: str) -> dict:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading YAML configuration: {str(e)}")
        config = {}

    return config