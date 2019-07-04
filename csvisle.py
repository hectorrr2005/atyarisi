import csv
import psycopg2
import os
import time
import shutil

conn = psycopg2.connect("dbname='atyarisi' user='user' host='localhost' password='password'")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_DIR = os.path.join(BASE_DIR, 'atyarisi')
DATACSV_DIR = os.path.join(PROJECT_DIR, 'datacsv')
hedefklasor = os.path.join(PROJECT_DIR, 'islenmisdata')


def replace_last(source_string, replace_what, replace_with):
    head, _sep, tail = source_string.rpartition(replace_what)
    return head + replace_with + tail


def csvprocess(filename):
    with open(os.path.join(DATACSV_DIR, filename), encoding="utf-8-sig", newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        firstrow = next(reader)
        atlar = []
        yas = []
        orjinBaba = []
        orjinAnne = []
        kilo = []
        jokey = []
        sahip = []
        antrenor = []
        kulvar = []
        agf = []
        handikap = []
        derece = []
        ganyan = []
        grup = []
        tur = []
        mesafe = []
        pist = []
        il = []
        tarih = []
        atsayilari = []
        kosuatsayilari = []
        hamgrup = []
        hamtur = []
        hammesafe = []
        hampist = []


        for row in reader:
            try:
                if "Kosu" in row[0]:
                    hamgrup.append(row[2].strip())
                    hamtur.append(row[1].strip())
                    for i in range(3, 8):
                        if "0m" in row[i]:
                            hammesafe.append(row[i].strip())
                            hampist.append(row[i+1].strip())
                atsayilari.append(int(row[0]))
                if "At İsmi" in row[1] or " TL" in row[1] or "Handikap" in row[1] or \
                        ". Yarış" in row[1] or "" == row[1] or "Maiden" in row[1] or \
                        "ŞARTLI" in row[1] or "A 3" in row[1]:
                    continue
                else:
                    yas.append(row[2].strip())
                    orjinBaba.append(row[3].strip())
                    orjinAnne.append(row[4].strip())
                    kilo.append(row[5].strip())
                    jokey.append(row[6].strip())
                    sahip.append(row[7].strip())
                    antrenor.append(row[8].strip())
                    kulvar.append(row[9][:2].strip())
                    if "" == row[10]:
                        agf.append("Yok")
                    else:
                        agf.append(row[10].strip())
                    if "" == row[11]:
                        handikap.append('0')
                    else:
                        handikap.append(row[11].strip())
                    if "Derecesiz" in row[12] or "Koşmaz" in row[12] or "" == row[12]:
                        derece.append("0:00.00")
                    else:
                        derece.append(row[12].replace('.', ':', 1))
                    if "" == row[13]:
                        ganyan.append('0')
                    else:
                        ganyan.append(row[13])
                    kgrow = row[1].replace(" KG", "*")
                    dbrow = kgrow.replace(" DB", "*")
                    skrow = dbrow.replace(" SK", "*")
                    gkrow = skrow.replace(" GKR", "*")
                    grow = gkrow.replace("*G*", "*")
                    bbrow = grow.replace(" BB", "*")
                    ogrow = bbrow.replace(" ÖG", "*")
                    krow = ogrow.replace("* K", "*")
                    yprow = krow.replace("* YP", "*")
                    ksrow = yprow.replace(" (Koşmaz)", "*")
                    yzrow = ksrow.replace(" K*", "*")
                    if yzrow[len(yzrow)-1] == "K" and yzrow[len(yzrow)-2] == " ":
                        mzrow = replace_last(yzrow, "K", "*")
                        temizrow = mzrow.replace("*", "").strip()
                        atlar.append(temizrow)
                    else:
                        temizrow = yzrow.replace("*", "").strip()
                        atlar.append(temizrow)
            except (IndexError, ValueError):
                continue

        for i in range(len(atlar)):
            il.append(firstrow[0])
            tarih.append(firstrow[2])

        siralama = []
        for sira in atsayilari:
            siralama.append(sira)
        atsayilari.append(1)

        for i in range(1, len(atsayilari)):
            if (atsayilari[i]) < (atsayilari[i - 1]):
                kosuatsayilari.append(atsayilari[i - 1])

        grup = sum([[s] * n for s, n in zip(hamgrup, kosuatsayilari)], [])
        tur = sum([[s] * n for s, n in zip(hamtur, kosuatsayilari)], [])
        mesafe = sum([[s] * n for s, n in zip(hammesafe, kosuatsayilari)], [])
        pist = sum([[s] * n for s, n in zip(hampist, kosuatsayilari)], [])

        mesafeint = []

        for item in mesafe:
            mesafeint.append(item.replace("m", ""))

        atdata = list(
            zip(siralama, atlar, yas, orjinBaba, orjinAnne, sahip, tarih, il, mesafeint, pist, grup, tur, kilo, jokey,
                antrenor, kulvar, handikap, derece, ganyan, agf))
    return atdata




def create_yarislar_table():
    sql = """ CREATE TABLE yarislar (id serial PRIMARY KEY, siralama integer, atlar varchar(50), 
            yas varchar(50), orjinbaba varchar(50), orjinanne varchar(50), sahip varchar(50),
            tarih date, il varchar(50),  mesafe integer, pist varchar(50), atyasgrup varchar(50),
            atgrup varchar(50), kilo varchar(50), jokey varchar(50), antrenor varchar(50),
            kulvar integer, handikap integer, derece time, ganyan varchar(50), agf varchar(50));"""
    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def insert_atlar_list(atlardata):
    sql = """INSERT INTO yarislar(siralama, atlar, yas, orjinBaba, orjinAnne, sahip, tarih, il, mesafe, pist, atyasgrup, atgrup,
            kilo, jokey, antrenor, kulvar, handikap, derece, ganyan, agf) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    try:
        cur = conn.cursor()
        for d in atlardata:
            cur.execute(sql, d)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


counter = 0
data_dir = os.listdir(DATACSV_DIR)

for dosya in data_dir:
    data = csvprocess(os.path.join(DATACSV_DIR, dosya))
    insert_atlar_list(data)
    shutil.move(os.path.join(DATACSV_DIR, dosya), hedefklasor)
    print(data)
    counter += 1
    print("İşlenen dosya "+dosya+" toplam: "+str(counter))
    time.sleep(1)

print(str(counter)+" dosya taşındı ve veritabanına işlendi.")
