import boto3
import pandas as pd

from ingest_patients_from_minio import (
    read_csv_smart,
    normalize_key,
    COLUMN_MAPPING,
    parse_age,
    validate_age_distribution_with_gx,
)

BUCKET = "hospital-raw"
KEY = "patients_03_colonnes_anglais_valeurs_invalides.csv"


def parse_age_for_demo(value):
    """
    Version volontairement démonstrative :
    - garde les âges négatifs pour que GX puisse les détecter ;
    - transforme les valeurs non numériques en None.
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def simulate_pipeline_filtering(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Simule la logique métier du pipeline :
    - les âges invalides ne sont pas insérés ;
    - ils sont considérés comme rejetés.
    """
    working_df = df.copy()

    working_df["age_parsed"] = working_df["age"].apply(parse_age)

    accepted_df = working_df[working_df["age_parsed"].notna()].copy()
    rejected_df = working_df[working_df["age_parsed"].isna()].copy()

    return accepted_df, rejected_df


print("\n==============================")
print("1. Connexion à MinIO")
print("==============================")

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

print(f"[MinIO] Bucket : {BUCKET}")
print(f"[MinIO] Fichier : {KEY}")

response = s3.get_object(Bucket=BUCKET, Key=KEY)
raw = response["Body"].read()


print("\n==============================")
print("2. Lecture du CSV brut")
print("==============================")

raw_df = read_csv_smart(raw)

print("[CSV brut]")
print(raw_df)


print("\n==============================")
print("3. Standardisation des colonnes")
print("==============================")

df = raw_df.copy()
df.columns = [
    COLUMN_MAPPING.get(normalize_key(col), normalize_key(col))
    for col in df.columns
]

print("[Colonnes standardisées]")
print(list(df.columns))

print("\n[DataFrame standardisé]")
print(df)


print("\n==============================")
print("4. Préparation de l'âge pour la démonstration GX")
print("==============================")

gx_demo_df = df.copy()
gx_demo_df["age"] = gx_demo_df["age"].apply(parse_age_for_demo)

print("[Âges envoyés à GX]")
print(gx_demo_df[["prenom", "nom", "age"]])


print("\n==============================")
print("5. Validation Great Expectations sur age")
print("==============================")

try:
    validate_age_distribution_with_gx(gx_demo_df, KEY)
    print("[GX] Validation terminée avec succès.")
except ValueError as error:
    print("[GX] Validation échouée volontairement pour la démonstration.")
    print(error)


print("\n==============================")
print("6. Simulation du comportement pipeline")
print("==============================")

accepted_df, rejected_df = simulate_pipeline_filtering(df)

print("\n[Patients qui seraient insérés en base]")
print(accepted_df[["prenom", "nom", "age", "age_parsed", "pathologie", "service"]])

print("\n[Patients qui seraient rejetés]")
print(rejected_df[["prenom", "nom", "age", "age_parsed", "pathologie", "service"]])


print("\n==============================")
print("7. Résumé")
print("==============================")

print(f"Fichier traité : {KEY}")
print(f"Lignes totales : {len(df)}")
print(f"Lignes acceptées : {len(accepted_df)}")
print(f"Lignes rejetées : {len(rejected_df)}")

print("\nConclusion :")
print("- Great Expectations contrôle la qualité de la colonne age.")
print("- Python applique ensuite la logique métier du pipeline.")
print("- Les âges invalides ne sont pas corrigés automatiquement.")
print("- Ils sont exclus de l'insertion en base.")