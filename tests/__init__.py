def main():
    import nose
    import sys
    
    from os import path
    for pth in ['.', './lib']:
        sys.path.insert(0, path.join(path.dirname(path.abspath(__file__)), pth))
    
    nose.main(sys.modules[__name__])

if __name__ == '__main__':
    main()
