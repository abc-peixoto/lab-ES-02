import csv
from pathlib import Path
from typing import Dict, List, Any

def load_github_data(csv_path: str) -> List[Dict[str, Any]]:
    """
    Carrega dados dos repositórios do GitHub a partir do CSV.
    """
    data = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            row['repo_name'] = row['full_name'].split('/')[-1]
            data.append(row)
    return data

def load_ck_summaries(summaries_dir: str) -> Dict[str, Dict[str, Any]]:
    """
    Carrega todos os CSVs de resumo do CK e organiza por repo_name.
    """
    summaries_path = Path(summaries_dir)
    ck_data = {}
    for csv_file in summaries_path.glob('*_quality_summary.csv'):
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            repo_name = None
            metrics = {}
            for row in reader:
                if repo_name is None:
                    repo_name = row['repo_name']
                metric = row['metric'].lower()
                for col in ['count', 'mean', 'median', 'stddev', 'min', 'max', 'p25', 'p75']:
                    if col in row:
                        metrics[f'{metric}_{col}'] = row[col]
            if repo_name:
                ck_data[repo_name] = metrics
    return ck_data

def merge_datasets(github_csv_path: str, summaries_dir: str, output_csv_path: str) -> None:
    """
    Mescla métricas de processo do GitHub com resumos de métricas de qualidade do CK.

    Args:
        github_csv_path: Caminho para repositories.csv
        summaries_dir: Caminho para o diretório summaries contendo *_quality_summary.csv
        output_csv_path: Caminho para o CSV de saída mesclado
    """
    # Carregar dados
    github_data = load_github_data(github_csv_path)
    ck_data = load_ck_summaries(summaries_dir)

    # Mesclar
    merged_data = []
    for repo in github_data:
        repo_name = repo['repo_name']
        merged_row = repo.copy()
        if repo_name in ck_data:
            merged_row.update(ck_data[repo_name])
        merged_data.append(merged_row)

    # Validação
    total_github = len(github_data)
    total_ck = len(ck_data)
    matched = sum(1 for row in merged_data if 'cbo_count' in row)

    print("Validação da mesclagem de dados:")
    print(f"  Repositórios do GitHub carregados: {total_github}")
    print(f"  Resumos do CK carregados: {total_ck}")
    print(f"  Correspondências bem-sucedidas: {matched}")
    print(f"  Perdidos na mesclagem (sem dados CK): {total_github - matched}")

    if matched == 0:
        print("  Aviso: Nenhum repositório correspondido com dados CK")

    # Determinar todas as colunas
    all_columns = set()
    for row in merged_data:
        all_columns.update(row.keys())
    all_columns = sorted(all_columns)

    # Salvar dados mesclados
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=all_columns)
        writer.writeheader()
        writer.writerows(merged_data)

    print(f"Conjunto de dados mesclado salvo em: {output_csv_path}")

if __name__ == "__main__":
    merge_datasets(
        github_csv_path='repositories.csv',
        summaries_dir='./workspace/summaries',
        output_csv_path='merged_data.csv'
    )