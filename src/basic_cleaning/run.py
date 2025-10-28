#!/usr/bin/env python
"""
Script für die Datenbereinigung
Lädt Rohdaten von W&B, bereinigt sie und lädt das Ergebnis wieder hoch
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):
    """
    Hauptfunktion für die Datenbereinigung
    """
    
    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    logger.info("Lade Rohdaten von W&B")
    # Artefakt herunterladen
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)
    
    logger.info(f"Daten geladen: {df.shape[0]} Zeilen")
    
    # Preisfilter anwenden
    logger.info(f"Filter Preise zwischen {args.min_price} und {args.max_price}")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    logger.info(f"Nach Preisfilter: {df.shape[0]} Zeilen")
    
    # Datum konvertieren
    logger.info("Konvertiere last_review zu datetime")
    df['last_review'] = pd.to_datetime(df['last_review'])
    
    # Geofilter für Ausreißer (wird für sample2 benötigt)
    logger.info("Wende geografischen Filter an")
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()
    logger.info(f"Nach Geofilter: {df.shape[0]} Zeilen")
    
    # Speichern und hochladen
    filename = "clean_sample.csv"
    df.to_csv(filename, index=False)
    
    logger.info(f"Lade bereinigte Daten zu W&B hoch: {filename}")
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(filename)
    run.log_artifact(artifact)
    
    logger.info("Fertig!")


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Datenbereinigung für NYC Airbnb Daten")

    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Name des Input-Artefakts in W&B",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name des Output-Artefakts",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Typ des Output-Artefakts",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Beschreibung des Output-Artefakts",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimaler Preis in Dollar",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximaler Preis in Dollar",
        required=True
    )

    args = parser.parse_args()

    go(args)
