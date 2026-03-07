from __future__ import annotations

"""Dictionnaire de donnees pour normaliser provinces, territoires et pathologies."""

import re
import unicodedata


def normalize_key(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return text.strip()


_TERRITORY_TABLE = """
Pcode Territoire Chef-lieu Province
5204 Aketi Aketi Bas-Uele
5207 Ango Ango Bas-Uele
5409 Aru Aru Ituri
5111 Bafwasende Bafwasende Tshopo
3202 Bagata Bagata Kwilu
5209 Bambesa Bambesa Bas-Uele
5110 Banalia Banalia Tshopo
4107 Basankusu Basankusu Équateur
5109 Basoko Basoko Tshopo
4503 Befale Befale Tshuapa
6109 Beni Beni Nord-Kivu
4102 Bikoro Bikoro Équateur
4502 Boende Boende Tshuapa
4506 Bokungu Bokungu Tshuapa
3309 Bolobo Bolobo Mai-Ndombe
4108 Bolomba Bolomba Équateur
4104 Bomongo Bomongo Équateur
5206 Bondo Bondo Bas-Uele
4405 Bongandanga Bongandanga Mongala
4306 Bosobolo Bosobolo Nord-Ubangi
4203 Budjala Budjala Sud-Ubangi
7306 Bukama Bukama Haut-Lomami
3205 Bulungu Bulungu Kwilu
4404 Bumba Bumba Mongala
4305 Businga Businga Nord-Ubangi
5202 Buta Buta Bas-Uele
9208 Dekese Dekese Kasaï
9106 Demba Demba Kasaï central
9102 Dibaya Dibaya Kasaï central
7205 Dilolo Dilolo Lualaba
9107 Dimbelenge Dimbelenge Kasaï central
4504 Djolu Djolu Tshuapa
5405 Djugu Djugu Ituri
5305 Dungu Dungu Haut-Uele
5307 Faradje Faradje Haut-Uele
3103 Feshi Feshi Kwango
6210 Fizi Fizi Sud-Kivu
4202 Gemena Gemena Sud-Ubangi
3211 Gungu Gungu Kwilu
3207 Idiofa Idiofa Kwilu
6206 Idjwi Idjwi Sud-Kivu
4505 Ikela Ikela Tshuapa
9206 Ilebo Ilebo Kasaï
4109 Ingende Ingende Équateur
3302 Inongo Inongo Mai-Ndombe
5402 Irumu Irumu Ituri
5105 Isangi Isangi Tshopo
7407 Kabalo Kabalo Tanganyika
6309 Kabambare Kabambare Maniema
6202 Kabare Kabare Sud-Kivu
8207 Kabeya-Kamwanga Kabeya-Kamwanga Kasaï oriental
8102 Kabinda Kabinda Lomami
7304 Kabongo Kabongo Haut-Lomami
3105 Kahemba Kahemba Kwango
6302 Kailo Kailo Maniema
6205 Kalehe Kalehe Sud-Kivu
7402 Kalemie Kalemie Tanganyika
7105 Kambove Kambove Haut-Katanga
8105 Kamiji Kamiji Lomami
7302 Kamina Kamina Haut-Lomami
9102 Kamonia Kamonia Kasaï
7303 Kaniama Kanyama Haut-Lomami
7207 Kapanga Kapanga Lualaba
2016 Kasangulu Kasangulu Kongo central
7107 Kasenga Kasenga Haut-Katanga
6312 Kasongo Kasongo Maniema
3107 Kasongo-Lunda Kasongo-Lunda Kwango
8308 Katako-Kombe Katako-Kombe Sankuru
8209 Katanda Katanda Kasaï oriental
9105 Kazumba Kazumba Kasaï central
3102 Kenge Kenge 2 Kwango
6313 Kibombo Kibombo Maniema
2019 Kimvula Kimvula Kongo central
7103 Kipushi Kipushi Haut-Katanga
3303 Kiri Kiri Mai-Ndombe
8306 Kole Kole Sankuru
9109 Kongolo Kongolo Tanganyika
4204 Kungu Kungu Sud-Ubangi
3306 Kutu Kutu Mai-Ndombe
3307 Kwamouth Kwamouth Mai-Ndombe
4205 Libenge Libenge Sud-Ubangi
4402 Lisala Binga Mongala
8304 Lodja Lodja Sankuru
8307 Lomela Lomela Sankuru
8109 Lubao Lubao Lomami
8309 Lubefu Lubefu Sankuru
6105 Lubero Lubero Nord-Kivu
7203 Lubudi Lubudi Lualaba
6306 Lubutu Lubutu Maniema
9204 Luebo Luebo Kasaï
8104 Luilu Luputa Lomami
9104 Luiza Luiza Kasaï central
4103 Lukolela Lukolela Équateur
2006 Lukula Lukula Kongo central
2010 Luozi Luozi Kongo central
8208 Lupatapata Lupatapata Kasaï oriental
8302 Lusambo Lusambo Sankuru
2017 Madimba Madimba Kongo central
5407 Mahagi Mahagi Ituri
4105 Mankanza Makanza Équateur
7305 Malemba-Nkulu Malemba-Nkulu Haut-Lomami
5403 Mambasa Mambasa Ituri
7406 Manono Manono Tanganyika
3213 Masi-Manimba Masi-Manimba Kwilu
6103 Masisi Masisi Nord-Kivu
2014 Mbanza-Ngungu Mbanza-Ngungu Kongo central
8206 Miabi Miabi Kasaï oriental
7108 Mitwaba Mitwaba Haut-Katanga
2004 Moanda Moanda Kongo central
7404 Moba Moba Tanganyika
4303 Mobayi-Mbongo Mobayi-Mbongo Nord-Ubangi
4507 Monkoto Monkoto Tshuapa
3311 Mushie Mushie Mai-Ndombe
7202 Mutshatsha Mutshatsha Lualaba
9207 Mweka Mweka Kasaï
6212 Mwenga Mwenga Sud-Kivu
8107 Ngandajika Ngandajika Lomami
5303 Niangara Niangara Haut-Uele
6102 Nyiragongo Kibumba Nord-Kivu
7410 Nyunzu Nyunzu Tanganyika
5103 Opala Opala Tshopo
3304 Oshwe Oshwe Mai-Ndombe
6307 Pangi Pangi Maniema
5208 Poko Poko Bas-Uele
3108 Popokabaka Popokabaka Kwango
6304 Punia Punia Maniema
7109 Pweto Pweto Haut-Katanga
5302 Rungu Rungu Haut-Uele
6112 Rutshuru Rutshuru Nord-Kivu
7104 Sakania Sakania Haut-Katanga
7206 Sandoa Sandoa Lualaba
2009 Seke-Banza Seke-Banza Kongo central
6204 Shabunda Shabunda Sud-Kivu
2011 Songololo Songololo Kongo central
2008 Tshela Tshela Kongo central
8203 Tshilenge Tshilenge Kasaï oriental
5108 Ubundu Ubundu Tshopo
6209 Uvira Uvira Sud-Kivu
6104 Walikale Walikale Nord-Kivu
6207 Walungu Walungu Sud-Kivu
5311 Wamba Wamba Haut-Uele
5309 Watsa Watsa Haut-Uele
5107 Yahuma Yahuma Tshopo
4304 Yakoma Yakoma Nord-Ubangi
3310 Yumbi Yumbi Mai-Ndombe
"""


def _extract_row(line: str) -> tuple[str, str, str, str] | None:
    """Parse une ligne brute de la table des territoires."""
    tokens = line.strip().split()
    if len(tokens) < 4:
        return None
    known_provinces = {
        "bas-uele",
        "ituri",
        "tshopo",
        "kwilu",
        "equateur",
        "tshuapa",
        "nord-kivu",
        "mai-ndombe",
        "mongala",
        "nord-ubangi",
        "sud-ubangi",
        "haut-lomami",
        "kasai",
        "kasai central",
        "lualaba",
        "haut-uele",
        "kwango",
        "sud-kivu",
        "maniema",
        "kasai oriental",
        "lomami",
        "tanganyika",
        "haut-katanga",
        "kongo central",
        "sankuru",
    }

    pcode = tokens[0]
    territoire = tokens[1]
    tail_two = " ".join(tokens[-2:])
    if normalize_key(tail_two) in {normalize_key(p) for p in known_provinces}:
        province = tail_two
        chef_tokens = tokens[2:-2]
    else:
        province = tokens[-1]
        chef_tokens = tokens[2:-1]
    chef_lieu = " ".join(chef_tokens).strip() if chef_tokens else territoire
    return pcode, territoire, chef_lieu, province


# Referentiel des territoires construit depuis la table RDC fournie.
TERRITORY_REFERENCE: list[dict[str, str]] = []
for raw_line in _TERRITORY_TABLE.strip().splitlines():
    if raw_line.lower().startswith("pcode"):
        continue
    row = _extract_row(raw_line)
    if row is None:
        continue
    TERRITORY_REFERENCE.append(
        {
            "pcode": row[0],
            "territoire": row[1],
            "chef_lieu": row[2],
            "province": row[3],
        }
    )


PROVINCE_ALIAS_TO_CANONICAL = {
    "equateur": "Equateur",
    "equateur province": "Equateur",
    "equateur ": "Equateur",
    "kongo central": "Kongo Central",
    "kongo-central": "Kongo Central",
    "kasai": "Kasai",
    "kasai central": "Kasai Central",
    "kasai central ": "Kasai Central",
    "kasai oriental": "Kasai Oriental",
    "nord kivu": "Nord Kivu",
    "sud kivu": "Sud Kivu",
    "nord ubangi": "Nord-Ubangi",
    "sud ubangi": "Sud-Ubangi",
    "bas uele": "Bas-Uele",
    "haut uele": "Haut-Uele",
    "haut katanga": "Haut-Katanga",
    "haut lomami": "Haut-Lomami",
    "mai ndombe": "Mai-Ndombe",
    "lualaba": "Lualaba",
    "lomami": "Lomami",
    "kwango": "Kwango",
    "kwilu": "Kwilu",
    "ituri": "Ituri",
    "tshopo": "Tshopo",
    "tshuapa": "Tshuapa",
    "sankuru": "Sankuru",
    "mongala": "Mongala",
    "kinshasa": "Kinshasa",
    "tanganyika": "Tanganyika",
    "maniema": "Maniema",
}


# Dictionnaires d'alias pour normalisation rapide territoire -> province.
TERRITORY_ALIAS_TO_CANONICAL: dict[str, str] = {}
TERRITORY_TO_PROVINCE: dict[str, str] = {}
for row in TERRITORY_REFERENCE:
    territory_key = normalize_key(row["territoire"])
    province_name = PROVINCE_ALIAS_TO_CANONICAL.get(normalize_key(row["province"]), row["province"])
    TERRITORY_ALIAS_TO_CANONICAL[territory_key] = row["territoire"]
    TERRITORY_TO_PROVINCE[territory_key] = province_name


PATHOLOGY_ALIAS_TO_CANONICAL = {
    "signes symptomes": "Signes & Symptomes",
    "signe symptome": "Signes & Symptomes",
    "mpox": "Mpox",
    "generalites": "Generalites",
    "paludisme": "Paludisme",
    "cholera": "Cholera",
    "ebola": "Ebola",
    "vih sida": "VIH/SIDA",
    "vih": "VIH/SIDA",
    "sida": "VIH/SIDA",
    "sante animale": "Sante animale",
    "tuberculose": "Tuberculose",
    "rougeole": "Rougeole",
    "typhoide": "Typhoide",
    "covid 19": "Covid-19",
    "covid19": "Covid-19",
    "amibiase": "Amibiase",
    "diarrhee": "Diarrhee",
    "fievre jaune": "Fievre jaune",
    "polio": "Polio",
}


def canonical_province_name(value: object) -> str:
    """Normalise un libelle province vers sa valeur canonique."""
    if value is None:
        return "Inconnu"
    raw = str(value).strip()
    key = normalize_key(raw)
    if not key:
        return "Inconnu"
    return PROVINCE_ALIAS_TO_CANONICAL.get(key, raw)


def canonical_territory_name(value: object) -> str:
    """Normalise un libelle territoire vers sa valeur canonique."""
    if value is None:
        return "Inconnu"
    raw = str(value).strip()
    key = normalize_key(raw)
    if not key:
        return "Inconnu"
    return TERRITORY_ALIAS_TO_CANONICAL.get(key, raw)


def province_from_territory(value: object) -> str | None:
    """Retourne la province d'un territoire si trouvable dans le referentiel."""
    key = normalize_key(value)
    if not key:
        return None
    return TERRITORY_TO_PROVINCE.get(key)


def canonical_pathology_name(value: object) -> str:
    """Normalise une pathologie/incident vers son libelle canonique."""
    if value is None:
        return "Non precise"
    raw = str(value).strip()
    key = normalize_key(raw)
    if not key:
        return "Non precise"
    return PATHOLOGY_ALIAS_TO_CANONICAL.get(key, raw)
