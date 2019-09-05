rem only needed if you add submodules etc..
<<<<<<< HEAD
sphinx-apidoc -o . ..\pydsm
make clean && make html
xcopy /s /e _build\* ..\docs
=======
rem sphinx-apidoc -o . ../pyhecdss
make clean && make html
xcopy /s /e _build/* ../docs
>>>>>>> 12e309c2eaab6ee849319d3175fbe5cf8b84ddfb
