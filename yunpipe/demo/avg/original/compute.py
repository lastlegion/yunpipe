inp = []

with open('input.txt') as f:
	inp.append(int(f.readline().stripe()))

with open('out.txt', 'w') as f:
	f.write(str(sum(inp) / float(len(inp))) + '\n')
