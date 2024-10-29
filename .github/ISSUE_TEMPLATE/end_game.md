---
name: Milestone Endgame
about: Ship a milestone!
title: 'v0.x.x endgame'
labels: 'kind/endgame'
assignees: ''
---

## DRIs

|         | DRI |
| ------- | --- |
| Endgame |     |
| QA      |     |
| Docs    |     |

## Planning Checklist

- [ ] Review the specific [GitHub Milestone](https://github.com/spiceai/gospice/milestones)

## Release Checklist

- [ ] All features/bugfixes to be included in the release have been merged to trunk
- [ ] Full test pass and update if necessary over [README.md](https://github.com/spiceai/gospice/blob/trunk/README.md)
- [ ] Full test pass and update if necessary over Docs
  - [ ] [docs.spiceai.org](https://docs.spiceai.org/sdks/golang)
  - [ ] [docs.spice.ai](https://github.com/spicehq/docs/blob/trunk/sdks/go.md)
- [ ] Test the [`gospice` sample](https://github.com/spiceai/samples/tree/trunk/client-sdk/gospice-sdk-sample) using the latest `trunk` SDK version.
- [ ] Update [release notes](https://github.com/spiceai/gospice/blob/trunk/docs/release_notes)
  - [ ] Ensure any external contributors have been acknowledged.
- [ ] Verify the version in `config.go` and `go.mod` are correct and match the milestone version.
- [ ] Run [Test CI](https://github.com/spiceai/gospice/actions/workflows/go.yml) and ensure it is green on the trunk branch.
- [ ] QA DRI sign-off
- [ ] Docs DRI sign-off
- [ ] Create a new branch `release-v[semver]` for the release from trunk. E.g. `release-v0.17.0-beta`
- [ ] Release the new version by creating and publishing a latest [GitHub Release](https://github.com/spiceai/gospice/releases/new) with the tag from the release branch. E.g. `v0.17.0-beta`.
- [ ] No publish steps are required as the SDK is installed directly from GitHub
- [ ] Run a test pass using the [`gospice` sample](https://github.com/spiceai/samples/tree/trunk/client-sdk/gospice-sdk-sample) using the latest published version.
- [ ] Update the version in `config.go` and `go.mod` to the next release version.
- [ ] The SDK release is added to the next [Spice release notes](https://github.com/spiceai/spiceai/tree/trunk/docs/release_notes)
