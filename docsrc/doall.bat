rem only needed if you add submodules etc..
call sphinx-apidoc -o . ..\pydsm
call make clean
call make html
call xcopy /y /s /e _build\* ..\docs
