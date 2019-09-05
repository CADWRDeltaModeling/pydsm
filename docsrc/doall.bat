rem only needed if you add submodules etc..
#sphinx-apidoc -o . ..\pydsm
make clean && make html
rem
xcopy /s /e _build\* ..\docs
