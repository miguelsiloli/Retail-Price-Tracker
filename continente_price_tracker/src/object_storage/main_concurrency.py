import concurrent.futures
from continente.catalog import process_and_save_categories
from pingo_doce.pingo_doce import parse_and_save_all_categories
from auchan.auchan import save_data_for_all_cgids

def main():
    # Define the tasks you want to run in parallel
    tasks = [
        ("Continente", process_and_save_categories, {"base_path": "data/raw/continente"}),
        ("Pingo Doce", parse_and_save_all_categories, {"categories": [
            "pingo-doce-lacticinios", "pingo-doce-bebidas",
            "pingo-doce-frescos-embalados", "pingo-doce-higiene-e-beleza",
            "pingo-doce-maquinas-e-capsulas-de-cafe", "pingo-doce-mercearia",
            "pingo-doce-refeicoes-prontas"
        ]}),
        ("Auchan", save_data_for_all_cgids, {
            "cgid_list": [
                "alimentacao-", "biologico-e-escolhas-alimentares",
                "limpeza-da-casa-e-roupa", "bebidas-e-garrafeira", "marcas-auchan", 
                "produtos-frescos", "Páginasbe_Antimanchas", "Páginasbe_Antiidade",
                "Páginasbe_Acne", "produtos-solares", "multivitaminicos", "PaginaSBE_pelesecaatopica",
                "maquilhagem"
            ],
            "prefn1": "soldInStores",
            "prefv1": "000",
            "sz": 212,
            "base_url": "https://www.auchan.pt/on/demandware.store/Sites-AuchanPT-Site/pt_PT/Search-UpdateGrid",
            "base_path": "data/raw/auchan"
        })
    ]

    # Use ThreadPoolExecutor to run tasks in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_task = {executor.submit(task[1], **task[2]): task[0] for task in tasks}

        for future in concurrent.futures.as_completed(future_to_task):
            task_name = future_to_task[future]
            try:
                result = future.result()
                print(f"{task_name} completed successfully.")
            except Exception as e:
                print(f"{task_name} generated an exception: {e}")

if __name__ == "__main__":
    main()
