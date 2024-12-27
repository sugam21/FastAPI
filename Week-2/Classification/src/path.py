from pathlib import Path

path: dict[str, Path] = {
    "model_save_dir": Path(".").resolve() / "saved" / "model"
}
