import bayeslite
import matplotlib.pyplot as plt
from bdbcontrib import query

plt.rcParams['font.weight']='bold'
plt.rcParams['axes.titleweight'] = 'bold'
plt.rcParams['axes.labelweight'] = 'bold'

bdb = bayeslite.bayesdb_open('1446849375.bdb')

sd_joint = query(bdb, 'SELECT * FROM sd_joint')
cc_joint = query(bdb, 'SELECT * FROM cc_joint')

sd_conditional = query(bdb, 'SELECT * FROM sd_conditional')
cc_conditional = query(bdb, 'SELECT * FROM cc_conditional')

fig, ax = plt.subplots()

ax.scatter(sd_joint['u'], sd_joint['x'], color='green',
    label='Data Generator (u,x)')
ax.scatter(sd_conditional['u'], [0] * 100, color='red',
    label='Data Generator (u|x=0)')

ax.scatter(cc_joint['u'], cc_joint['x'], color='blue',
    label='crosscat (u,x)')
ax.scatter(cc_conditional['u'], [0.05] * 100, color='purple',
    label='crosscat (u|x=0)')

ax.set_xlabel('u')
ax.set_ylabel('x')
ax.set_title('''Joint And Conditional Simulations from
    u ~ Uniform[0,2pi], x|u ~ Normal[sin(u), 0.1]''')
ax.grid()
ax.legend()
plt.show()
