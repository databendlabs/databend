[https://datafuse.rs](https://datafuse.rs) 

# How to quickly test the website in local

## Dependencies
```markdown
cd website/datafuse
pip install .
pip install \
mkdocs-minify-plugin>=0.3 \
mkdocs-redirects>=1.0
```

## Run
```markdown
mkdocs serve
```

If you modify the files in [src](src), please run `npm run build` to rebuild material first.

## Note

Made with [Material for MkDocs](https://github.com/squidfunk/mkdocs-material)


