from concurrent.futures import ProcessPoolExecutor,as_completed
import statsmodels.api as sm
from init import *
from loguru import logger
import argparse
from multiprocessing import cpu_count


parser = argparse.ArgumentParser()
parser.add_argument('--factors',help='list of factors, seperated by comma')
parser.add_argument('--window',help='last number of months to calculate beta',type=int)
parser.add_argument('-o','--output',
        help='output file name',default='adj_ret.pq')
parser.add_argument('--debug',help='run when debugging',action='store_true')
parser.add_argument('-cpu',help='cpu percentage',type=int,default=100)

args = parser.parse_args()
output_filename = args.output

f = read('factors.csv')

dates = sorted(f.date.drop_duplicates().tolist())

factors = args.factors.split(',')
logger.info(f"factors used: {factors}")
window = args.window

cal_date = dict()
for i,x in enumerate(dates):
    if i>window:
        cal_date[x] = pd.DataFrame(dates[i-window:i],columns=['date'])

msf = rch('''select permno,toInt32(a.date/100) as date,ret*100-rf as eret, b.*
            from msf a left join factors b on toInt32(a.date/100)=b.date''')

msf = msf[['permno','date','eret']+factors].dropna()

def reg(g):
    if len(g)>window*0.8:
        X = sm.add_constant(g[factors])
        res = sm.OLS(g.eret,X).fit()
        # if args.debug:
            # logger.debug(f"{res.params}")
        return res.params

def cal(date):
    if date in cal_date:
        a = msf.merge(cal_date[date]).groupby(['permno']).apply(reg)
        if len(a)==0:
            logger.warning(f"{date} has no return data")
            return
        a.reset_index()
        a['date'] = date
        if args.debug:
            logger.debug(f"{a.head()}")
        return a.dropna()

with ProcessPoolExecutor(int(cpu_count()*args.cpu/100)) as p:
    res = []
    for date in dates:
        res.append(p.submit(cal,date))
    res = as_completed(res)
    x = pd.concat([a.result() for a in res if a.result() is not None]).dropna()
    x.to_parquet('_adj_ret.pq',index=False)

for f in factors:
    x.rename({f:'b_'+f},axis=1,inplace=True)

logger.info("finish computing betas, now combining results...")

msf = msf.merge(x,on=['permno','date'])
msf['adj_eret'] = msf['eret']

for f in factors:
    msf['adj_eret'] += -msf['b_'+f] * msf[f]

logger.info("outputing file...")
msf.to_parquet(output_filename,index=False)
