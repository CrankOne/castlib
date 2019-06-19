# The CastLib Package

The package is designed for automated synchronization of medium-scale data
among multiple storaging filesystem-like back-ends in a recursive manner with
clear and highly configurable behaviour.

Historically its main purpose was to provide the centralized and managable
approach for synchronization of the experimental data statistics acquired
by NA64 experiment on SPS (CERN, Geneva, 2015 - nowadys).

The major targeting back-end is the CERN Advanced STORage manager
([CASTOR](http://castor.web.cern.ch/)), the service offering hierarchical
management system for high-persistent data.

## Issues

- For developers, 13/09/017: Note about `sqlamp`.
The `castlib` uses the [`sqlamp` package](https://bitbucket.org/angri/sqlamp)
offering quite neat and unobtrusive solution for introducing
materialized paths to filesystem entries model implemented as adjacency list.
This package, however, has [known issue](https://bitbucket.org/angri/sqlamp/pull-requests/1/made-subtree-moving-compatible-with-ms-sql/diff)
that wasn't fixed by the maintainer despite of clear solution. So far, the
workaround for that is to add the `"parent_id_field"` string in the
iteration list within the `for`-clause in `site-packages/sqlamp/__init__.py:1478`
of your virtualenv after `sqlamp` has been installed.
For future releases, we will either migrate this package inside `CastLib`
package itself getting rid of this dependency, or use it if maintainer will
fix it, or just introduce our own materialized path mixin.

## License (MIT)

> Copyright (c) 2017 Renat R. Dusaev <crank@qcrypt.org>
> 
> Permission is hereby granted, free of charge, to any person obtaining a copy of
> this software and associated documentation files (the "Software"), to deal in
> the Software without restriction, including without limitation the rights to
> use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
> the Software, and to permit persons to whom the Software is furnished to do so,
> subject to the following conditions:
> 
> The above copyright notice and this permission notice shall be included in all
> copies or substantial portions of the Software.
> 
> THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
> IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
> FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
> COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
> IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
> CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

