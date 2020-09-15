
mkdir vendor
cd vendor

wget https://files.pythonhosted.org/packages/78/f8/b77a2603a4aa184412576127c2846f7a69b42e6c1596d791c817643880d0/pandas-1.1.1-cp38-cp38-manylinux1_x86_64.whl -O pandas.whl
unzip pandas.whl
wget https://files.pythonhosted.org/packages/c2/76/73df80caf7affbe4b4f4a3b69a9a8f10b3b2acbb8179ad5bb578daaee56c/numpy-1.19.1-cp38-cp38-manylinux1_x86_64.whl -O numpy.whl
unzip numpy.whl

pip install --target . sqlalchemy
pip install --target . git+https://github.com/aplbrain/grand
pip install --target . git+https://github.com/aplbrain/grandiso-networkx
pip install --target . networkx
pip install --target . pytz

rm -r *.whl *.dist-info __pycache__