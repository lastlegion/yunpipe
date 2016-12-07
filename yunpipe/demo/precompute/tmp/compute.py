inp = []

with open('input.txt') as f:
    for line in f:
	    inp.append(int(line.strip()))

with open('sq.txt', 'w') as f:
	for n in inp:
		f.write(str(n * n) + '\n')

with open('cb.txt', 'w') as f:
	for n in inp:
		f.write(str(n * n) + '\n')
