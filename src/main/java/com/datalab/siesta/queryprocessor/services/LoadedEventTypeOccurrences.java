package com.datalab.siesta.queryprocessor.services;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class LoadedEventTypeOccurrences {
    private Map<String,Map<String,Long>> eventTypeOccurrences;
}
