
pip install --upgrade setuptools wheel
python setup.py sdist bdist_wheel

pip install twine
twine upload dist/* -u $PYPI_USERNAME -p $PYPI_PASSWORD
