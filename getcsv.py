from datetime import date, timedelta
import urllib.request
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_DIR = os.path.join(BASE_DIR, 'atyarisi')
DATACSV_DIR = os.path.join(PROJECT_DIR, 'datacsv')
PROGRAMCSV_DIR = os.path.join(PROJECT_DIR, 'programcsv')


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


start_date = date(2019, 6, 30)
end_date = date(2019, 7, 5)

bugun = date.today().strftime("%d/%m/%Y")
#
tarihler = []
iller = ['1', '2', '3', '4','6', '8', '9','5']

def tarihliste(start_date, end_date):
    for single_date in daterange(start_date, end_date):
        tarihler.append(single_date.strftime("%d/%m/%Y"))

tarihliste(start_date, end_date)

def ikiTarihArasiDataCek(tarihler, iller):
    for tarih in tarihler:
        for il in iller:
            url = "http://www.tjk.org/TR/YarisSever/Info/GetCSV/GunlukYarisSonuclari?SehirId="+ il +"&QueryParameter_Tarih="+ tarih
            dtarih = tarih.replace("/", "")
            dosyaadi = 'csv'+dtarih+'_'+il+'.csv'
            try:
                urllib.request.urlretrieve(url, os.path.join(DATACSV_DIR, dosyaadi))
            except urllib.request.HTTPError:
                continue
    print("İşlem Tamamlandı")

def ikiTarihArasiProgramCek(tarihler, iller):
    for tarih in tarihler:
        for il in iller:
            url = "http://www.tjk.org/TR/YarisSever/Info/GetCSV/GunlukYarisProgrami?SehirId="+ il +"&QueryParameter_Tarih="+ tarih
            dtarih = tarih.replace("/", "")
            dosyaadi = 'csv' + dtarih + '_' + il + '.csv'
            try:
                urllib.request.urlretrieve(url, os.path.join(PROGRAMCSV_DIR, dosyaadi))
            except urllib.request.HTTPError:
                continue
    print("İşlem Tamamlandı")

def bugunDataCek(bugun, iller):
    for il in iller:
        url = "http://www.tjk.org/TR/YarisSever/Info/GetCSV/GunlukYarisSonuclari?SehirId=" + il + "&QueryParameter_Tarih=" + bugun
        dtarih = bugun.replace("/", "")
        dosyaadi = 'csv' + dtarih + '_' + il + '.csv'
        try:
            urllib.request.urlretrieve(url, os.path.join(DATACSV_DIR, dosyaadi))
        except urllib.request.HTTPError:
            continue
    print("İşlem Tamamlandı")

def bugunProgramCek(bugun, iller):
    for il in iller:
        url = "http://www.tjk.org/TR/YarisSever/Info/GetCSV/GunlukYarisProgrami?SehirId=" + il + "&QueryParameter_Tarih=" + bugun
        dtarih = bugun.replace("/", "")
        dosyaadi = 'csv' + dtarih + '_' + il + '.csv'
        try:
            urllib.request.urlretrieve(url, os.path.join(PROGRAMCSV_DIR, dosyaadi))
        except urllib.request.HTTPError:
            print(bugun+' tarihli '+il+' kodlu il için csv bulunamadı.')
            continue
    print("İşlem Tamamlandı")


#url = "http://www.tjk.org/TR/YarisSever/Info/GetCSV/GunlukYarisSonuclari?SehirId=&QueryParameter_Tarih=22/12/2018"

#urllib.request.urlretrieve(url, 'output2.csv')
#bugunDataCek(bugun, iller)
#bugunProgramCek(bugun, iller)
ikiTarihArasiProgramCek(tarihler, iller)
#ikiTarihArasiDataCek(tarihler, iller)