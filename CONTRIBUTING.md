Contribution Agreement
======================
As a contributor, you represent that the code you submit is your original work or that of your employer (in which case you represent you have the right to bind your employer).  By submitting code, you (and, if applicable, your employer) are licensing the submitted code to LinkedIn and the open source community subject to the BSD 2-Clause license. 

Tips for Getting Your Pull Request (PR) Accepted
===========================================
Make sure all new features are tested and the tests pass -- i.e., a submitted PR should have already been tested for existing and new unit tests.
Bug fixes must include a test case demonstrating the error that it fixes.
Open an issue first and seek advice for your change before submitting a PR. Large features which have never been discussed are unlikely to be accepted.
Do not create a PR with "work-in-progress" (WIP) changes.
Use clear and concise titles for submitted PRs and issues.
Each PR should be linked to an existing issue corresponding to the PR (see [PR template](docs/pull_request_template.md)).
If there are no existing issues about a PR, create one before submitting the PR.
We strongly encourage the use of recommended code-style for the project (see [code-style.xml](docs/code-style.xml)).
A pre-commit CheckStyle hook can be run by adding `./checkstyle/checkstyle-pre-commit` to your `.git/hooks/pre-commit` script.