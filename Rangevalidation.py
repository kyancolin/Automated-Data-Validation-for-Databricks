import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

# Functie om een bestand te lezen (denk aan het openen van een bestand)
def lees_bestand(pad):
    # os.path.splitext haalt de extensie van het bestand op
    _, ext = os.path.splitext(pad)
    ext = ext.lower().lstrip('.')  # Verwijdert de punt van de extensie en maakt alles lowercase aanemend dat python case gevoelig is
    
    # Controleer het type bestand en lees het correct in
    if ext == 'json':
        return spark.read.format("json").option("multiline", "true").load(pad)
    elif ext == 'csv':
        return spark.read.csv(pad, header=True, inferSchema=True)
    elif ext == 'parquet':
        return spark.read.parquet(pad)
    elif ext == 'xml': 
        return spark.read.format('com.databricks.spark.xml').option('rowTag', 'row').load(pad)
    else:
        raise ValueError(f"Verkeerd bestandtype: {ext}")

# Functie om te achterhalen welke datasets in de kolom leeftijd te hoog of te laag zijn 
def controleer_range(df, kolom, min_waarde, max_waarde):

    fouten = []  # Lijst waarin alle fouten in terug te vinden zijn 
    df = df.withColumn("rij", monotonically_increasing_id() + 1) # Rij toveogen in de dataframe om foutieve rijen terug te vinden
    
    # if statement om te kijken of de kolom in de dataframe zit (leeftijd)
    if kolom in df.columns:
        # Filteren op kolom (leeftijd), kijken of datasets kleiner zijn dan 16 OR groter dan 80, zo ja worden deze geselecteerd en verzameld
        buiten_bereik = df.filter((col(kolom) < min_waarde) | (col(kolom) > max_waarde)).select("rij", kolom).collect()
        # Er worden een paar dingen toegevoegd aan de array om ervoor te zorgen dat het duidelijker is voor de lezer
        for rij in buiten_bereik:
            fouten.append({ # Voegt een nieuwe rij toe aan de fouten array
                "kolom_naam": kolom, # De kolom naam
                "rij": rij["rij"], # De rij waarop de fout zit
                "waarde": rij[kolom] # De waarde die buiten de range valt
                })
    return fouten

def toon_fouten(fouten):
    if fouten:
        df_fouten = spark.createDataFrame(fouten)
        display(df_fouten)
    else:
        print("Geen fouten gevonden.")

# Bestandspad naar het bestand
bestand_pad = "/Volumes/data/data/landing/Kyan/performance_employees.json"

# Gebruik de lees_bestand functie om het bestand te laden in een DataFrame
df = lees_bestand(bestand_pad)
df.cache()  

# Controleer op fouten en sla ze op in de fouten lijst
fouten = controleer_range(df, 'leeftijd', 16, 80)

# Toon de fouten netjes
toon_fouten(fouten)

# Toon de originele DataFrame, voor het geval we alles willen zien
display(df)