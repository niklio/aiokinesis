pip install --upgrade setuptools wheel
python setup.py sdist bdist_wheel

pip install twine
twine upload --repository-url https://test.pypi.org/legacy/ dist/* -u $PYPI_USERNAME -p $PYPI_PASSWORD
