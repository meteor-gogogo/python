def test1(i, a):
    a[i] = i
    return a


if __name__ == '__main__':
    a = dict()
    for i in range(1,10):
        b = test1(i, a)
        print(b)
