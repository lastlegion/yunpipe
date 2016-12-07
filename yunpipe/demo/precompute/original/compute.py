inp = []

with open('input.txt') as f:
	inp.append(int(f.readline().stripe()))

with open('sq.txt', 'w') as f:
	for n in inp:
		f.write(str(n * n) + '\n')

with open('cb.txt', 'w') as f:
	for n in inp:
		f.write(str(n * n) + '\n')
