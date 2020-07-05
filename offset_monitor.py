import subprocess


# this example show a plan to close the spider
def check(order):
    res = subprocess.check_output(order, shell=True).decode('utf-8')
    res = res.split('\n')
    results = res[2].split(' ')
    for i in results:
        if i == '':
            results.remove(i)
    offset = int(results[3])
    log_size = int(results[4])
    if offset == log_size or log_size - offset <= 1:
        return 'finished'
    return 'failed'
