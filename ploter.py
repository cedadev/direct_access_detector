import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

df = pd.read_json('sizes.json')
print (df)
size = df/1e12
#size.drop('/', axis=1, inplace=True)

#ax = size.plot.area(linestyle='None')
ax = size.plot.line()
ax.set_ylabel('Volume opened (TB)')
ax.set_xlabel('CEDA Archive')
ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x, p: format(int(x), ',')))
plt.xlim([datetime(2021,7,1), datetime(2021,9,30)])

plt.savefig("size.png")


num = pd.read_json('num.json')
#num.drop('/', axis=1, inplace=True)

#ax = num.plot.area(linestyle='None')
ax = num.plot.line()
ax.set_ylabel('Files opened')
ax.set_xlabel('CEDA Archive')
ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x, p: format(int(x), ',')))
plt.xlim([datetime(2021,7,1), datetime(2021,9,30)])
plt.savefig("num.png")
