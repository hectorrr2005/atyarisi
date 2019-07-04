from sqlalchemy import create_engine
import pandas as pd
from pandas import ExcelWriter

engine = create_engine('postgresql://admin:56tyghbn@localhost:5432/atyarisi')

programsql = """
    SELECT atlar, orjinbaba, orjinanne, sahip, il, 
       mesafe, pist, atyasgrup, atgrup, kilo, jokey, antrenor, kulvar
    FROM public.program where tarih = '2019-06-30' and il = 'İstanbul'
    """

yarissql = """
        SELECT * FROM public.yarislar where il = 'Adana' and siralama = '1'
        """
'''
dfprogram = pd.read_sql(programsql, engine)
dfyaris = pd.read_sql(yarissql, engine)

analiz = pd.merge(dfprogram, dfyaris, on='atlar', how='right')
writer = ExcelWriter('analiz2018.xlsx')
dfprogram.to_excel(writer, 'Sheet 1')
dfyaris.to_excel(writer, 'Sheet 2')
analiz.to_excel(writer, 'Sheet 3')
writer.save()
'''
'''
sonuclar = pd.read_sql_table('yarislar', engine)

birinciler = sonuclar[sonuclar.siralama == 1]
writer = ExcelWriter('analizjokey.xlsx')
analiz = birinciler.groupby(['il', 'jokey']).siralama.agg('count')
analiz.to_excel(writer, 'Sheet 1')
writer.save()
'''
'''
sonuclar = pd.read_sql(yarissql, engine)
writer = ExcelWriter('adana.xlsx')
sonuclar.to_excel(writer, 'Sayfa 1')
writer.save()
'''
writer = ExcelWriter('adanajokey.xlsx')
adana = pd.read_excel('adana.xlsx')
analiz = adana.groupby(['jokey', 'siralama']).siralama.agg('count')
analiz.to_excel(writer, 'Sheet 1')
writer.save()
