from scrapper.catalog import process_and_save_categories
from scrapper.pingo_doce import parse_and_save_all_categories
from scrapper.auchan import save_data_for_all_cgids

# continente
process_and_save_categories()

# pingo doce
# Example usage
categories = [
    "pingo-doce-lacticinios", "pingo-doce-bebidas",
    "pingo-doce-frescos-embalados", "pingo-doce-higiene-e-beleza",
    "pingo-doce-maquinas-e-capsulas-de-cafe", "pingo-doce-mercearia",
    "pingo-doce-refeicoes-prontas", "pingo-doce-cozinha-e-limpeza", "pingo-doce-congelados"
]

parse_and_save_all_categories(categories)
cgid_list = [
    "alimentacao-", "biologico-e-escolhas-alimentares",
    "limpeza-da-casa-e-roupa", "bebidas-e-garrafeira", "marcas-auchan",
    "saude-e-bem-estar", "brinquedos-papelaria-livraria",
    "Feira-de-Beleza"

]
prefn1 = "soldInStores"
prefv1 = "000"
sz = 212
base_url = "https://www.auchan.pt/on/demandware.store/Sites-AuchanPT-Site/pt_PT/Search-UpdateGrid"

save_data_for_all_cgids(cgid_list,
                       prefn1,
                       prefv1,
                       sz,
                       base_url,
                       base_path="data/raw/auchan")
