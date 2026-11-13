"""Ship shared Python modules to Spark executors in standalone cluster mode."""
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

root = Path(__file__).resolve().parents[1]
target = root / 'data' / 'traffic-python.zip'
target.parent.mkdir(exist_ok=True)
with ZipFile(target, 'w', ZIP_DEFLATED) as archive:
    for package in ('common',):
        for path in (root / package).glob('*.py'):
            archive.write(path, path.relative_to(root))
print(f'Packaged {target}')
