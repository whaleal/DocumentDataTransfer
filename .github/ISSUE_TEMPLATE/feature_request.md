name: Request Feature
description: Suggest exciting and meaning features to DocumentDataTransfer
title: "[Feature][Module Name] Feature title"
labels: ["feature"]
body:
  - type: markdown
    attributes:
      value: |
        Please describe what you want in English, thanks!

  - type: checkboxes
    attributes:
      label: Already searched before asking?
      description: >
        Please make sure to search in the [feature](https://github.com/whaleal/DocumentDataTransfer/issues?q=is%3Aissue+label%3A%22Feature%22) first
        to see whether the same feature was requested already.
      options:
        - label: >
            I had searched in the [feature](https://github.com/whaleal/DocumentDataTransfer/issues?q=is%3Aissue+label%3A%22Feature%22) and found no
            similar feature requirement.
          required: true

  - type: textarea
    attributes:
      label: Usage Scenario
      description: Please describe usage scenario of this feature.
    validations:
      required: true

  - type: textarea
    attributes:
      label: Description
      description: Please describe feature as much detail as possible
      placeholder: >
        describe what you are trying to achieve and why, rather than telling how you might implement this feature if you are willing to submit pull request.
    validations:
      required: true


  - type: checkboxes
    attributes:
      label: Are you willing to submit a PR?
      description: >
        DocumentDataTransfer appreciates community-driven contribution and we love to bring new contributors in.
      options:
        - label: Yes, I am willing to submit a PR!

  - type: checkboxes
    attributes:
      label: Code of Conduct
      description: |
        The Code of Conduct helps create a safe space for everyone. We require that everyone agrees to it.
      options:
        - label: |
            I agree to follow this project's [Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/)
          required: true

  - type: markdown
    attributes:
      value: "Thanks for completing our form, and we will reply you as soon as possible."

