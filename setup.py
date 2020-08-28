import distutils
from distutils.core import setup
import glob

bin_files = glob.glob("bin/*")  

# The main call
setup(name='opstoolkit',
      version ='0.2.2',
      license = "GPL",
      packages = ['opstoolkit','pipelines'],
      package_dir = {'': 'python'},
      scripts = bin_files,
      data_files=[('ups',['ups/opstoolkit.table'])]
      )           
                 

