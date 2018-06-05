python -m pip install --user --upgrade setuptools wheel
python setup.py sdist bdist_wheel

python -m pip install --user --upgrade twine
twine upload --repository-url https://test.pypi.org/legacy/ dist/* -u $PYPI_USERNAME -p $PYPI_PASSWORD
