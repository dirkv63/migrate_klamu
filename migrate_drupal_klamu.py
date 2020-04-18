from lib import my_env
from lib import mysqlstore as drupal
from lib import klamu_store
from lib.klamu_store import *

cfg = my_env.init_env("klamu_migrate", __file__)
db = os.getenv('KLAMU_DB')
logging.info("Start application")
ds = drupal.DirectConn()
klamu = klamu_store.init_session(db=db)

# Collect CD Information
uitgevers = ds.get_uitgevers()
identificaties = ds.get_identificaties()
cds = ds.get_cds()
for rec in cds:
    cd = Cd(
        id=rec['nid'],
        titel=rec['title'],
        created=rec['created'],
        modified=rec['changed']
    )
    try:
        cd.identificatie = identificaties[rec['nid']]
    except KeyError:
        pass
    try:
        cd.uitgever = uitgevers[rec['nid']]
    except KeyError:
        pass
    klamu.add(cd)

# Collect Komponist Informatie
voornamen = ds.get_voornamen()
komponisten = ds.get_komponisten()
for rec in komponisten:
    komponist = Komponist(
        id=rec['nid'],
        naam=rec['title'],
        created=rec['created'],
        modified=rec['changed']
    )
    try:
        komponist.voornaam = voornamen[rec['nid']]
    except KeyError:
        komponist.voornaam =''
    klamu.add(komponist)

# Collect Kompositie Information
kompositie_to_cd = ds.get_kompositie_to_cd()
kompositie_to_komponist = ds.get_kompositie_to_komponist()
volgnummers = ds.get_volgnummers()
uitvoerders = ds.get_uitvoerders()
dirigentnamen = ds.get_dirigentnamen()
dirigentvoornamen = ds.get_dirigentvoornamen()
komposities = ds.get_komposities()
for rec in komposities:
    kompositie = Kompositie(
        id=rec['nid'],
        naam=rec['title'],
        created=rec['created'],
        modified=rec['changed'],
        cd_id=kompositie_to_cd[rec['nid']]
    )
    try:
        kompositie.komponist_id = kompositie_to_komponist[rec['nid']]
    except KeyError:
        pass
    try:
        kompositie.volgnummer = volgnummers[rec['nid']]
    except KeyError:
        pass
    try:
        kompositie.uitvoerders = uitvoerders[rec['nid']]
    except KeyError:
        pass
    try:
        kompositie.dirigent_naam = dirigentnamen[rec['nid']]
        kompositie.dirigent_voornaam = dirigentvoornamen[rec['nid']]
    except KeyError:
        pass
    klamu.add(kompositie)
klamu.commit()
ds.close()
logging.info("End application")
