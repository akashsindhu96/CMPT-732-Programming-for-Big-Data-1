def compare(x, y):
    if x[0] >= y[0]:
        return x
    else:
        return y


print(compare((int(5), 'akash'), (int(4), 'ankit')))

t = ('as', (1, 'asd'))
print(t[1][0])