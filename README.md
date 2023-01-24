# locus-ql

### Deployment:
* **Deployment to `/dev`** - triggered when the `main` branch is updated or when a tag with a name fits the respective `dev-*` pattern, such as `dev-202301120142`.

* **Deployment to `/prod`** - triggered when a tag with a name that fits the respective `prod-*` pattern, such as `prod-202301120142`.

* **Publishing an alpha version to NPM** - triggered when a tag with a name that fits the respective `v*-alpha*` pattern, such as `v0.1.0-alpha.1`

* **Publishing to NPM** - triggered when a tag with a name that fits the respective `v*`(skips `v*-*`) pattern, such as `v0.1.0`
