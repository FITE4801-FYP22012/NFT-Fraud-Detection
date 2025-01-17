import sagemaker
from src.model_build import jobs
from sagemaker.inputs import TrainingInput
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.parameters import ParameterString
from sagemaker.workflow.steps import ProcessingStep, TrainingStep


def get_pipeline(iam_role, cfg):
    # PROCESSING STEP
    processor = jobs.get_processor(iam_role=iam_role, cfg=cfg["processing"])
    step_process = ProcessingStep(
        name="PrepareData",
        processor=processor,
        code=cfg["processing"]["entry_point"],
        inputs=[
            sagemaker.processing.ProcessingInput(
                source=cfg["processing"]["s3_input_data_uri"],
                destination=cfg["processing"]["parameters"]["input_folder"],
            )
        ],
        outputs=[
            sagemaker.processing.ProcessingOutput(
                name="output-1",
                source=cfg["processing"]["parameters"]["output_folder"],
                destination=cfg["processing"]["s3_output_data_uri"],
            )
        ],
    )

    # GET TRAINING INPUT FROM PROCESSING OUTPUT
    training_input = TrainingInput(
        s3_data=step_process.properties.ProcessingOutputConfig.Outputs[
            "output-1"
        ].S3Output.S3Uri,
        content_type="application/x-parquet",
    )

    # TRAINING STEP
    estimator = jobs.get_estimator(iam_role=iam_role, cfg=cfg["training"])
    step_train = TrainingStep(
        name="TrainEvaluateRegister",
        estimator=estimator,
        inputs={"input": training_input},
    )

    # PIPELINE INSTANCE
    pipeline = Pipeline(
        name=cfg["pipeline"]["name"], parameters=[], steps=[step_process, step_train]
    )

    return pipeline
