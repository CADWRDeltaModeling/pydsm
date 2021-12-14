rem only needed if you add submodules etc..
call conda activate dev_pydsm
call sphinx-apidoc -f -o . ..\pydsm
call make clean
call make html
rem call xcopy /y /s /e _build\* ..\docs
call xcopy /y /s /e _build\* ..\..\pydsm-gh