import sys

from locust import HttpUser, task
from river.datasets import synth

# Generate a synthetic dataset to use for load/data quality
# testing; see https://riverml.xyz/0.14.0/api/datasets/synth/Agrawal/
# for details on the construction/history of this data set
agrawal = synth.Agrawal(seed=1729)
iter = agrawal.take(sys.maxsize)


class GenerateSyntheticData(HttpUser):
    @task
    def generate_data(self):
        payload, y = next(iter)
        self.client.post("/collect", json=payload)
