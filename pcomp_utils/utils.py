def batch_generator(array, batch_size=1000):
    for i in range(0, array.shape[0], batch_size):
        yield array[i:i+batch_size]