from init import *
ff = read('ff5.csv')
mom = read('F-F_Momentum_Factor.csv')[['date','mom']]
ni  = dfname(read('Portfolios_Formed_on_NI.csv')).astype(float).assign(ni=lambda s:s.hi_10 - s.lo_10)[['date','ni']]
resvar = dfname(read('Portfolios_Formed_on_RESVAR.csv')
               ).astype(float).assign(resvar=lambda s:s.lo_10 - s.hi_10)[['date','resvar']]

liq = read('liq.csv')[['date','liq']]
liq['liq'] = liq.liq*100
bab = read('bab.csv')
q = read('q_factor.csv')
f = ff.merge(mom).merge(liq,how='left').merge(bab).merge(ni).merge(resvar).merge(q,how='left')
f.to_csv('factors.csv')

create_table(f,'factors','date',load_data=True)
