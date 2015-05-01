# BayesDB experiments

***Notes for tests**:
- Each test should be comprehensible from its plot alone.
- Tests should return a pass/fail value so they can be used for automated testing.

## Tests

### Haystacks
BayesDB should find dependencies where they exist and should find independencies otherwise. Create a noisy state with a couple of pairs of dependent columns---use correlation or zero-correlation pattern with strength in [0, 1]. Analyze. Every n iterations retrieve the pairwise dependence probabilities. Plot the probabilities as a function of iterations (seaborn and matplotlib will be required to plot, obviously). Also, increase the number of distractor (noise) columns.

### Recovers original densities
BayesDB should be able to recover (simulate) the density of the data it analyzes. Train on zero-correlation data sets (four dots, sine wave, ring, diamond) and simulate x, y.

## TODO
[ ] Haystacks
[ ] Error bars shrink with iterations
[ ] Recovers original densities
[ ] Fills in the blanks
