import json
from time import time
from analyzer.base_analyzer import BaseAnalyzer, BaseAnalyzerConfig
from integrations.slack_integration import SlackGetterConfig
from utils import get_time_in_seconds, get_time_bins


class MetricAnalyzerConfig(BaseAnalyzerConfig):
    def __init__(self, source):
        super().__init__()
        self.name = "Metric Analyzer"
        self.source = source


class MetricAnalyzer(BaseAnalyzer):
    def __init__(self, config):
        super().__init__()
        self.source_configs = config.source
        self.config = config

    def prepare_data(self, source_name):
        getter = self.source_executors[source_name](
            config=self.source_configs[source_name]
        )
        metadata, df = getter.crawl_workspace()
        return metadata, df

    def _get_top_n_channel(self, channel_metrics, by_duration, metadata, n=3):
        metrics = list(channel_metrics.values())
        metrics = sorted(
            metrics, key=lambda x: x[by_duration]["total_activity"], reverse=True
        )

        n = min(n, len(metrics))
        metrics = metrics[:n]
        output_json = {i["id"]: i[by_duration] for i in metrics}

        for key, value in output_json.items():
            value["channel_name"] = (metadata["channel_metadata"][key]["name"],)

        output_json["by_duration"] = by_duration
        output_json["top_n"] = n
        return output_json

    def _get_top_user_in_channel(self, df, duration, n=5):
        top_users = df.sent_by.value_counts()
        n = min(n, len(top_users))
        top_users = top_users[:n]
        output_dict = {
            key: int(value) for key, value in zip(top_users.index, top_users.array)
        }
        output_dict["duration"] = str(n)
        output_dict["top_n"] = n
        return output_dict

    def _unattended_post_per_channel(self, group):
        group = group[group["is_engaging_post"] == True]
        return

    def _all_channels(self, metadata):
        return {
            "num_channels": len(metadata["channel_metadata"].keys()),
            "channels": list(metadata["channel_metadata"].keys()),
        }

    def _total_activity_per_channel(self, group):
        return self.activity_in_duration(group, -1)

    def activity_in_duration(self, df, duration="1d"):
        time_duration = get_time_in_seconds(duration)
        df = df[df["ts"] > time() - time_duration]
        return {
            "total_activity": int(df.total_activity.sum()),
            "num_replies": int(df.reply_count.sum()),
            "num_files": int(df.num_files.sum()),
            "num_reactions": int(df.reactions.sum()),
            "duration": duration,
        }

    def trending_posts(self, group, n=3):
        group["activity"] = group.apply(
            lambda x: x["reactions"] + x["reply_count"], axis=1
        )
        final_df = group.sort_values(by=["activity"], ascending=False)

        n = min(n, len(final_df))
        final_df = final_df[:n]
        output_dict = {
            row["id"]: int(row["activity"]) for i, row in final_df.iterrows()
        }
        output_dict["duration"] = str(n) + "d"
        return output_dict

    def process_community_metrics(self, df, metadata):

        grouped_channel = df.groupby("channel")
        output = {}
        channel_metrics = {}
        for channel_id, group in grouped_channel:
            top_users_in_channel = self._get_top_user_in_channel(group, 3)
            new_activity = self.activity_in_duration(group, 1)
            total_activity = self._total_activity_per_channel(group)
            trending_posts = self.trending_posts(group)
            channel_metrics[channel_id] = {
                "id": channel_id,
                "name": metadata["channel_metadata"][channel_id]["name"],
                "users": metadata["channel_metadata"][channel_id]["users"],
                "top_users_in_channel": top_users_in_channel,
                "new_activity": new_activity,
                "total_activity": total_activity,
                "trending_posts": trending_posts,
            }

        output["channel_metrics"] = channel_metrics
        output["top_n_channel"] = self._get_top_n_channel(
            channel_metrics, "total_activity", metadata
        )
        output["all_channel"] = self._all_channels(metadata)

        return output

    def _post_per_user(self, df, duration="all"):
        time_duration = get_time_in_seconds(duration)
        df = df[df["ts"] > time() - time_duration]
        return {"num_posts": len(df), "duration": duration}

    def _reactions_on_user_post(self, df, duration="all"):
        time_duration = get_time_in_seconds(duration)
        df = df[df["ts"] > time() - time_duration]
        return {"num_posts": int(df.reactions.sum()), "duration": duration}

    def reply_on_user_post(self, df, duration="all"):
        time_duration = get_time_in_seconds(duration)
        df = df[df["ts"] > time() - time_duration]
        num_replies = int(df["reply_count"].astype(int).sum())
        num_reactions = int(df["reactions"].astype(int).sum())
        return {"num_posts": num_replies + num_reactions, "duration": duration}

    def user_distinct_channels(self, df):
        return {"distinct_channels": df["channel"].nunique()}

    def _user_activity_period_bins(self, df, period="3d"):
        if len(df) == 0:
            return {"frequencies": [], "time_period": 0}
        df, labels = get_time_bins(df, period)
        activities = []
        previous_activity = 0
        for label in labels:
            user_activities = {}
            sliced_df = df[df["time_bins"] == label]
            if len(sliced_df) == 0:
                activities.append(user_activities)
                continue
            activity_agg = int(sliced_df.total_activity.sum())
            user_activities["total_activity"] = activity_agg
            user_activities["activity_change_rate"] = activity_agg - previous_activity
            user_activities["time_period"] = label
            user_activities["start_ts"] = sliced_df.ts.iloc[0]
            previous_activity = activity_agg
            activities.append(user_activities)
        return activities

    def _user_activity_in_period(self, df, period):
        if len(df) == 0:
            return {"activity": "", "time_period": 0}
        time_period = get_time_in_seconds(period)
        current_time = time()
        df_trimmed = df[df["ts"] > current_time - time_period]
        if len(df_trimmed) == 0:
            return {"activity": "", "time_period": time_period}
        return {
            "begin_ts": df_trimmed.ts.iloc[0],
            "activity": int(df_trimmed.total_activity.sum()),
            "time_duration": period,
        }

    def process_user_metrics(self, df, metadata):
        groups = df.groupby("sent_by")
        user_metrics = {}
        for user_id, df_group in groups:
            post_count = self._post_per_user(df_group, "all")
            reaction_count = self._reactions_on_user_post(df_group, "all")
            replies_count = self.reply_on_user_post(df_group)
            user_channels_count = self.user_distinct_channels(df_group)
            user_activity_in_interval = self._user_activity_in_period(df_group, "3d")

            user_metrics[user_id] = {
                "post_user_made": post_count,
                "reactions_user_get": reaction_count,
                "replies_user_get": replies_count,
                "user_channels_count": user_channels_count,
                "user_total_activity": user_activity_in_interval,
                "name": metadata["user_metadata"][user_id]["full_name"],
            }

        user_data = {"metrics": user_metrics}
        return user_data

    def analyze_metrics(self, source_name):
        metadata, df = self.prepare_data(source_name)
        community_metrics = self.process_community_metrics(df, metadata)
        user_metrics = self.process_user_metrics(df, metadata)
        return community_metrics, user_metrics


if __name__ == "__main__":
    config = MetricAnalyzerConfig(
        source={
            "slack": SlackGetterConfig(
                token_id="xoxb-2370583994869-2413498212784-UBMvJ1ozsR1DEniG3uw8oz8C"
            )
        }
    )
    metric_analyzer = MetricAnalyzer(config=config)
    channel_metrics = metric_analyzer.analyze_metrics("slack")
    with open("../debug/out.json", "w") as f:
        json.dump(channel_metrics, f)
