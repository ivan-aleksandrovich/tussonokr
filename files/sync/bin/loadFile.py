from plugins import artixinterchange
import time, sys

if __name__ == '__main__':
    import propertyReader
    if len(sys.argv)>1 and sys.argv[1]:
        artixinterchange.upload(sys.argv[1], str(int(time.time())))