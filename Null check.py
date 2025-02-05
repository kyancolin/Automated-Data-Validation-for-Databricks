import os
from pyspark.sql import SparkSession # Handig voor grotere datasets
from pyspark.sql.functions import col, monotonically_increasing_id

# Functie om een bestand te lezen op basis van het bestandstype
def lees_bestand(pad):
    # Bepaal de extensie van het bestand
    _, ext = os.path.splitext(pad) # "_" Pakt de het eerste stukje "DOUBLE value" "," scheidt deze waardes zodat de variabele in ext wordt opgeslagen 
    ext = ext.lower().lstrip('.')
    # Lees het bestand afhankelijk van het bestandstype
    if ext == 'json':
        return spark.read.format("json").option("multiline", "true").load(pad)
    elif ext == 'csv':
        return spark.read.csv(pad, header=True, inferSchema=True)
    elif ext == 'parquet':
        return spark.read.parquet(pad)
    elif ext == 'xml':
        return spark.read.format('com.databricks.spark.xml').option('rowTag', 'row').load(pad)
    
# Functie om null-waarden te identificeren in verplichte kolommen
def controleer_nulls(df): # Dit maakt het gemakkelijker om te controleren op null-waarden in specifieke kolommen.
    verplichte_kolommen = ['id']  # Definieer verplichte kolommen, in andere woorden deze kolommen mogen GEEN null-waardes hebben
    fouten = {} # Dictionary aanmaken om alle fouten op te slaan 
    # Voeg een rijnummer kolom toe voor overzichtelijkheid
    df = df.withColumn("rij", monotonically_increasing_id()+1) #withcolumn functie die kolom toevoegt aan DF, monotonically_increasing_id
    for kolom in verplichte_kolommen:
        if kolom in df.columns: # controleert op aanwezigheid van de veprlichte kolom  
            # Zoek naar null-waarden in de verplichte kolom
            nulls = df.filter(col(kolom).isNull()).select("rij").collect() #  Controleert of een kolom een null-waarde bevat, filter(selecteer), select om de rijnummers op te halen om in de DF te verwerken, collect verwerkt alles in een lijst denk bijvoorbeeld aan de dictionary "fouten"
            if nulls:
                fouten[kolom] = [rij["rij"] for rij in nulls] # 
                # Alle null-waardes wordenn toegevoegd aan de eerder gemaakte dictionary fouten 
                # Kolom, oftewel de naam van de kolom waarin de fout is gevonden
        else: 
            print(f"Aangegeven Kolom niet gevonden")
    return fouten # Fouten tonen

# Functie om de fouten overzichtelijk weer te geven
# True or False implementeren bij de if statement 

def toon_fouten(fouten):
    if fouten:
        output = []
        for kolom, rijen in fouten.items():
            output.append({"kolom_naam": kolom, "rijen": ", ".join(map(str, rijen))})
        df_fouten = spark.createDataFrame(output)
        df_fouten.createOrReplaceTempView("fouten_view")
        display(df_fouten)
    else:
        print("Geen fouten gevonden.")
  # else statement met een  bericht als er geen fouten zijn 

# Pad naar het bestand
bestand_pad = "/Volumes/data/data/landing/Kyan/performance_employees.json"
df = lees_bestand(bestand_pad)  # Lees het bestand
df.cache()  # Cache de DataFrame voor betere prestaties
fouten = controleer_nulls(df)  # Identificeer fouten in verplichte kolommen
toon_fouten(fouten)  # Toon de gevonden fouten
display(df)