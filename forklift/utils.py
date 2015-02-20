def batcher(generator, batch_size):
    num_in_batch = 0
    batch = []
    for item in generator:
        batch.append(item)
        num_in_batch += 1
        if num_in_batch >= batch_size:
            num_in_batch = 0
            yield batch
            batch = []
    if num_in_batch > 0:
        yield batch


def get_or_create_efid(asid, appid):
    # TODO: implement? web service?
    return asid
