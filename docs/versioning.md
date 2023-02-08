# Versioning

Currently the project is in early stages, so expect the API to frequently change.

Starting with version 1.0, this project will follow [Semantic Versioning](http://semver.org/) with the
following caveats:

-   Only the public API (i.e. the objects imported into the evercas
    module) will maintain backwards compatibility between MINOR version
    bumps.
-   Objects within any other parts of the library are not guaranteed to
    not break between MINOR version bumps.

With that in mind, it is recommended to only use or import objects from
the main module, evercas.
