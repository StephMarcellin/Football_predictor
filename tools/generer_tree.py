import os
from pathlib import Path

# Dossiers à ignorer absolument pour la clarté
EXCLUDE_DIRS = {'.venv', 'venv', 'etc', 'share', 'include', 'Lib', 'Scripts', 'data',
                'pycache', '.git', '.idea', '.vscode', 'mlruns'}

def generate_tree(startpath):
    output = []
    for root, dirs, files in os.walk(startpath):
        # On filtre les dossiers à ignorer
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS and not d.startswith('__')]
        
        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * level
        output.append(f"{indent}{os.path.basename(root)}/")
        sub_indent = ' ' * 4 * (level + 1)
        for f in files:
            if not f.startswith('.'): # Ignore les fichiers cachés
                output.append(f"{sub_indent}{f}")
    return "\n".join(output)

if __name__ == "__main__":
    cwd = os.getcwd()
    ROOT_DIR = Path(__file__).resolve().parent.parent
    print(f"Génération de la structure du projet à partir de : {cwd}")
    tree_text = generate_tree(cwd)
    with open(ROOT_DIR / "tools/structure_projet.txt", "w", encoding="utf-8") as f:
        f.write(tree_text)
    print("Fichier structure_projet.txt généré avec succès !")