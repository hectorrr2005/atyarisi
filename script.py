from datetime import datetime, timedelta
import time


tarih = '00:01:23,91'
mesafe = 1400
hedef = 1900
def hesapla(zaman, mesafe, hedefmesafe):
    sekil = "%H:%M:%S,%f"
    zamanobje = datetime.strptime(zaman, sekil)
    print(type(zamanobje))
    dakika = int(zamanobje.minute)
    saniye = int(zamanobje.second)
    milisaniye = int(zamanobje.microsecond/10000)
    toplam = float((((dakika * 60)*100)+(saniye * 100)+milisaniye)/100)
    yuzmetre = toplam / (mesafe/100)
    derecesn = yuzmetre * (hedefmesafe / 100) 
    dk = int(derecesn/60)
    sn = derecesn - (dk*60)
    sl = (sn - int(sn)) * 100
    slint = int(sl)
    tahmin = '{}:{},{}'.format(dk,int(sn),slint)
    return tahmin

    
print(hesapla(tarih, mesafe, hedef))