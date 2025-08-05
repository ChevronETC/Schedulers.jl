using Documenter, Schedulers

makedocs(sitename = "Schedulers", modules = [Schedulers])

deploydocs(repo = "git@github.com:ChevronETC/Schedulers.jl.git")
