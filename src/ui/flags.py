"""Country flags for the Nation codes (FIFA-style trigrams, as FBref shows them).

Flags are images from flagcdn.com, not emoji: Windows browsers show emoji flags
as two plain letters. England, Scotland, Wales and Northern Ireland use
flagcdn's gb-eng / gb-sct / gb-wls / gb-nir; Kosovo uses xk.
"""

from __future__ import annotations

FLAG_URL = "https://flagcdn.com/w40/{code}.png"

# FIFA trigram -> flagcdn code (ISO 3166-1 alpha-2, lower case, plus the UK nations).
# Covers every trigram src/soccer_data_loader.NATION_CODES produces and every one
# in the FBref seasons (tests/test_flags.py checks the first).
FIFA_TO_FLAG: dict[str, str] = {
    "AFG": "af", "ALB": "al", "ALG": "dz", "AND": "ad", "ANG": "ao", "ATG": "ag", "ARG": "ar",
    "ARM": "am", "ARU": "aw", "AUS": "au", "AUT": "at", "AZE": "az", "BRB": "bb", "BLR": "by",
    "BEL": "be", "BLZ": "bz", "BEN": "bj", "BER": "bm", "BOL": "bo", "BIH": "ba", "BRA": "br",
    "VGB": "vg", "BUL": "bg", "BFA": "bf", "BDI": "bi", "CMR": "cm", "CAN": "ca", "CPV": "cv",
    "CAY": "ky", "CTA": "cf", "CHA": "td", "CHI": "cl", "CHN": "cn", "TPE": "tw", "COL": "co",
    "COM": "km", "CGO": "cg", "COD": "cd", "CRC": "cr", "CRO": "hr", "CUB": "cu", "CUW": "cw",
    "CYP": "cy", "CZE": "cz", "CIV": "ci", "DEN": "dk", "DMA": "dm", "DOM": "do", "ECU": "ec",
    "EGY": "eg", "SLV": "sv", "ENG": "gb-eng", "EQG": "gq", "ERI": "er", "EST": "ee",
    "ETH": "et", "FRO": "fo", "FIN": "fi", "FRA": "fr", "GUF": "gf", "GAB": "ga", "GAM": "gm",
    "GEO": "ge", "GER": "de", "GHA": "gh", "GRE": "gr", "GRN": "gd", "GLP": "gp", "GUM": "gu",
    "GUA": "gt", "GUI": "gn", "GNB": "gw", "GUY": "gy", "HAI": "ht", "HON": "hn", "HKG": "hk",
    "HUN": "hu", "ISL": "is", "IND": "in", "IDN": "id", "IRN": "ir", "IRQ": "iq", "IRL": "ie",
    "ISR": "il", "ITA": "it", "JAM": "jm", "JPN": "jp", "JOR": "jo", "KAZ": "kz", "KEN": "ke",
    "PRK": "kp", "KOR": "kr", "KVX": "xk", "LAO": "la", "LVA": "lv", "LBN": "lb", "LES": "ls",
    "LBR": "lr", "LBY": "ly", "LIE": "li", "LTU": "lt", "LUX": "lu", "MKD": "mk", "MAD": "mg",
    "MWI": "mw", "MAS": "my", "MLI": "ml", "MLT": "mt", "MTQ": "mq", "MTN": "mr", "MRI": "mu",
    "MEX": "mx", "MDA": "md", "MNE": "me", "MSR": "ms", "MAR": "ma", "MOZ": "mz", "NAM": "na",
    "NED": "nl", "NCL": "nc", "NZL": "nz", "NCA": "ni", "NIG": "ne", "NGA": "ng",
    "NIR": "gb-nir", "NOR": "no", "PAK": "pk", "PLE": "ps", "PAN": "pa", "PAR": "py",
    "PER": "pe", "PHI": "ph", "POL": "pl", "POR": "pt", "PUR": "pr", "QAT": "qa", "ROU": "ro",
    "RUS": "ru", "RWA": "rw", "KSA": "sa", "SCO": "gb-sct", "SEN": "sn", "SRB": "rs",
    "SEY": "sc", "SLE": "sl", "SVK": "sk", "SVN": "si", "SOM": "so", "RSA": "za", "SSD": "ss",
    "ESP": "es", "SRI": "lk", "SKN": "kn", "LCA": "lc", "VIN": "vc", "SUR": "sr", "SWE": "se",
    "SUI": "ch", "SYR": "sy", "STP": "st", "TAN": "tz", "THA": "th", "TLS": "tl", "TOG": "tg",
    "TRI": "tt", "TUN": "tn", "TUR": "tr", "TCA": "tc", "VIR": "vi", "USA": "us", "UGA": "ug",
    "UKR": "ua", "URU": "uy", "UZB": "uz", "VEN": "ve", "VIE": "vn", "WAL": "gb-wls",
    "YEM": "ye", "ZAM": "zm", "ZIM": "zw",
}


def flag_url(nation: object) -> str | None:
    """The flag image URL for a Nation code, or None when there is no flag for it."""
    code = FIFA_TO_FLAG.get(str(nation).strip().upper()) if nation is not None else None
    return FLAG_URL.format(code=code) if code else None
