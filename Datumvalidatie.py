import os
from pyspark.sql.functions import col, to_date, monotonically_increasing_id, lit
from pyspark.sql import SparkSession

# Deze functie leest een bestand vanaf een opgegeven pad(ext) en bepaalt het type op basis van de extensie.
# Afhankelijk van de extensie wordt het bestand met de juiste methode gelezen.
def lees_bestand(pad):
    # Haal de bestandsextensie op en zet deze om naar kleine letters voor uniformiteit.
    _, ext = os.path.splitext(pad)
    ext = ext.lower().lstrip('.')
    
    if ext == 'json':
        return spark.read.format("json").option("multiline", "true").load(pad)
    elif ext == 'csv':
        return spark.read.csv(pad, header=True, inferSchema=True)
    elif ext == 'parquet':
        return spark.read.parquet(pad)
    elif ext == 'xml':
        return spark.read.format('com.databricks.spark.xml').option('rowTag', 'row').load(pad)
    # Optioneel: want je gaat ervan uit dat het de juiste bestandtype is 
    # else:
    #    raise ValueError(f"Verkeerd bestandstype: {ext}")

# Deze functie controleert datums in een DataFrame.
def datum_check(df):
    # Voeg een unieke rij-ID toe aan elke rij in de foutieve dataframe
    df = df.withColumn("rij", monotonically_increasing_id())
    
    # Probeer datums te vergelijken doormiddel van parsen in een specifiek formaat "dd-MM-yyyy"
    # en als het geen correcte datumstructuur is, dan wordt de kolom met de foutieve datums gevuld met NULL
    df = df.withColumn("datum_object", to_date(col("datum_in_dienst"), "dd-MM-yyyy"))
    
    # Filter de rijen waarin de datums niet correct geparst konden worden (NULL waarden).
    fouten = df.filter(col("datum_object").isNull()).select("rij", "datum_in_dienst")
    
    # Retourneer zowel het DataFrame met de nieuwe kolom als het DataFrame met fouten.
    return df, fouten

# Deze functie toont de fouten
def toon_fouten(fouten):
    # Voeg een kolom toe om het probleem te beschrijven 
    # Probleem is al duidelijk gezien de script dus verkeerde datum structuur
    fouten = fouten.withColumn("probleem", lit("Foutief datumstructuur"))
    
    # Gebruik een display-methode om de fouten overzichtelijk te presenteren.
    display(fouten)

# Pad naar het bestand dat gecontroleerd moet worden.
pad = "/Volumes/data/data/landing/Kyan/performance_employees.json"

# Lees het bestand in een DataFrame. Deze stap is essentieel om de gegevens te verwerken.
df = lees_bestand(pad)

# Controleer de datums in het DataFrame.
df, fouten = datum_check(df)

# Toon de fouten die zijn gevonden tijdens de controle.
toon_fouten(fouten)

# Optioneel: De gehele bestand tonen
display(df)