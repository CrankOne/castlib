# The CastLib Package

The main purpose of this package is to provide automated solution for recursive
synchronization of the filesystem entries (UNIX FS) with CASTOR storage offered
by CERN internal service.

Despite it has a prototype status for now and still is used in frame of
NA64 experiment only, the API itself was assembled to face generic needs.

More doc will be available as soon as principal part of DB ORM will be
stabilized.

## Note about `sqlamp`

As for 13/09/017:
For developers: both the CastLib3 and the sV-resources server use the
[`sqlamp` package](https://bitbucket.org/angri/sqlamp) offering quite neat and
inobtrusive solution for introducing [materialized paths]() to filesystem
entries model implemented as [adjacency list](). This package, however,
has [known issue](https://bitbucket.org/angri/sqlamp/pull-requests/1/made-subtree-moving-compatible-with-ms-sql/diff)
that wasn't fixed by the maintainer despite of known solution. So far, the
cleanest workaround for that is to add the `"parent_id_field"` string in the
iterating list within the `for`-clause in `site-packages/sqlamp/__init__.py:1478`
of your virtualenv after `sqlamp` has been installed.

For future releases, we will get either migrate this package inside `CastLib`
package itself getting rid of this dependency, or use it 

