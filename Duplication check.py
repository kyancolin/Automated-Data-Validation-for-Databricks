import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Functie om een bestand te lezen op basis van het bestandstype
def lees_bestand(pad):
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
    else:
        raise ValueError(f"Deze bestandtype wordt niet ondersteund bestandtype: {ext}")

# Functie om dubbele records te identificeren
def controleer_duplicaten(df, kolommen):
    df_dupes = df.groupBy(kolommen).count().filter(col("count") > 1)
    return df_dupes

# Functie om de gevonden dubbele records overzichtelijk weer te geven
def toon_duplicaten(duplicaten, kolom_naam):
    if duplicaten.count() > 0:
        output = []
        for row in duplicaten.collect():
            output.append({
                "kolom_naam": kolom_naam,
                "waarde": row[kolom_naam],
                "aantal": row["count"]
            })

        df_duplicaten = spark.createDataFrame(output)
        df_duplicaten = df_duplicaten.select("kolom_naam", "waarde", "aantal")
        df_duplicaten.createOrReplaceTempView("duplicaten_view")
        display(df_duplicaten)
    else:
        print("Geen dubbele records gevonden.")

# Pad naar het bestand
bestand_pad = "/Volumes/data/data/landing/Kyan/medewerkers2.csv"

# Lees het bestand
df = lees_bestand(bestand_pad)
df.cache()

# Kolommen die uniek moeten zijn
unieke_kolommen = ['id']  # Pas deze aan naar de kolommen die uniek moeten zijn in je dataset

# Controleer op duplicaten in de opgegeven kolommen
duplicaten = controleer_duplicaten(df, unieke_kolommen)

# Toon de gevonden duplicaten
toon_duplicaten(duplicaten, unieke_kolommen[0])  # Geef de naam van de kolom door

display(df)

# In de huidige output zie ik eerst aantal en daarna kolom_naam, dit wil ik niet. Ik wil graag eerst kolom_naam en daarna aantal.
# Daarnaast is het niet overzichtelijk, voor de lezer, ik wil mijn kolommen aanpassen. Allereerst wil ik "kolom_naam" terugzien, dit moet daadwerkelijk de naam zijn. Vervolgens moet de waarde dat dubbel voorkomt als tweede output en als laatste de aantal keer dat het voorkomt. 

# Om je script volledig te automatiseren en flexibel te maken,
# kun je de bestandsnaam dynamisch bepalen,
# bijvoorbeeld door een directory te scannen en de nieuwste bestanden