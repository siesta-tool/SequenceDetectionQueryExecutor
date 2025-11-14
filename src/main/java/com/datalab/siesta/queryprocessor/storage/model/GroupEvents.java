package com.datalab.siesta.queryprocessor.storage.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class GroupEvents {
    private int group_id;
    private List<EventModel> events;
}
