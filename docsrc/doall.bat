rem only needed if you add submodules etc..
rem sphinx-apidoc -o . ..\pydsm
make clean && make html
rem
xcopy /y /s /e _build\* ..\docs
