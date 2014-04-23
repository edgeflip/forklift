import sys, os

print sys.path
sys.path.append(os.path.abspath(os.curdir))
print sys.path
